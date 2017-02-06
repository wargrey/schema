#lang digimon

(provide (all-defined-out) (struct-out msg:schema))

(require "virtual-sql.rkt")
(require "message.rkt")
(require "syntax.rkt")

(define-type Schema schema)
(struct schema () #:prefab)

(define-syntax (define-table stx)
  (syntax-parse stx #:datum-literals [:]
    [(_ tbl #:as Table #:with primary-key (~optional entity-attribute-mode) ([field : DataType constraints ...] ...)
        (~or (~optional (~seq #:check record-contract:expr) #:name "#:check" #:defaults ([record-contract #'#true]))
             (~optional (~seq #:serialize serialize) #:name "#:serialize")
             (~optional (~seq #:deserialize deserialize) #:name "#:deserialize")) ...)
     (with-syntax* ([___ (format-id #'id "...")]
                    [(table dbtable) (parse-table-name #'tbl)]
                    [(PkType [pkid pkey] ...) (parse-primary-key #'primary-key)]
                    [eam (if (attribute entity-attribute-mode) (id->sql #'entity-attribute-mode) #'#false)]
                    [([view? [rowid table-rowid] ...]
                      [(:field table-field field-contract FieldType MaybeNull on-update [defval ...]) ...]
                      [(column-id column table-column DBType column-guard column-not-null column-unique) ...]
                      [check-fields table.sql]
                      [table? table-row? msg:schema:table make-table-message table->hash hash->table]
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

                (define table.sql : (-> Symbol (-> Statement) Statement)
                  (let ([dict : (HashTable Symbol Statement) (make-hasheq)])
                    (lambda [which mksql] (hash-ref! dict which mksql))))
                
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
                         (schema-throw [exn:schema 'contract `((struct . table) (expected . ,expected) (given . ,given))]
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
                  (when (and view?) (throw exn:fail:unsupported 'create-table "cannot create a temporary view"))
                  (define (mksql) : Virtual-Statement
                    (create-table.sql silent? dbtable '(pkey ...) eam '(column ...) '(DBType ...)
                                      '(column-not-null ...) '(column-unique ...)))
                  (query-exec dbc (table.sql (if silent? 'create-if-not-exists 'create) mksql)))

                (define (insert-table [dbc : Connection] [selves : (U Table (Listof Table))]
                                      #:or-replace? [replace? : Boolean #false]) : Void
                  (define (mksql) : Virtual-Statement (insert-into.sql replace? dbtable eam '(column ...)))
                  (when (and view?) (throw exn:fail:unsupported 'insert-table "cannot insert records into a temporary view"))
                  (define dbsys : Symbol (dbsystem-name (connection-dbsystem dbc)))
                  (define insert.sql : Statement (table.sql (if replace? 'replace 'insert) mksql))
                  (for ([row (if (list? selves) (in-list selves) (in-value selves))])
                    (define column-id : SQL-Datum (racket->sql (table-column row) dbsys)) ...
                    (cond [(false? eam) (query-exec dbc insert.sql column-id ...)]
                          [else (query-exec dbc insert.sql column-id ... (serialize row))])))
       
                (define (delete-table [dbc : Connection] [selves : (U Table (Listof Table))]) : Void
                  (define (mksql) : Virtual-Statement (delete.sql dbtable '(pkey ...)))
                  (cond [(and view?) (throw exn:fail:unsupported 'delete-table "cannot delete records from a temporary view")]
                        [else (for ([self (if (list? selves) (in-list selves) (in-value selves))])
                                (query-exec dbc (table.sql 'delete-table-by-rowid mksql) (table-rowid self) ...))]))

                (define-syntax (select-table stx) (syntax-case stx [] [(_ argl ___) #'(sequence->list (in-table argl ___))]))
                (define (in-table [dbc : Connection]
                                  #:where [where : (Option PkType) #false]
                                  #:fetch [size : (U Positive-Integer +inf.0) 1]) : (Sequenceof (U Table exn))
                  (define (sql-ref [which : Symbol] [method : Symbol]) : Statement
                    (table.sql which (thunk (simple-select.sql method dbtable '(pkey ...) eam '(column ...)))))
                  (define (read-table [sexp : SQL-Datum]) : (U Table exn)
                    (with-handlers ([exn? (λ [[e : exn]] e)])
                      (deserialize sexp)))
                  (define (read-by-pks [pks : (Vectorof SQL-Datum)]) : (U Table exn)
                    (with-handlers ([exn? (λ [[e : exn]] e)])
                      (define record : (Listof Any)
                        (for/list ([sql (in-vector (apply query-row dbc (sql-ref 'select-row 'row) (vector->list pks)))]
                                   [guard (in-list (list column-guard ...))])
                          (sql->racket sql guard)))
                      (cond [(table-row? record) (apply unsafe-table record)]
                            [else (schema-throw [exn:schema 'assertion `((struct . table) (record . ,pks) (got . ,record))]
                                                'select-table "maybe the database is penetrated")])))
                  (cond [(and where)
                         (match-define (list pkid ...) (if (sql-datum? where) (list where) where))
                         (cond [eam (sequence-map read-table (in-query dbc #:fetch size (sql-ref 'select-where 'byrowid) pkid ...))]
                               [else (in-list (map read-by-pks (query-rows dbc (sql-ref 'select-where 'byrowid) pkid ...)))])]
                        [(and eam) (sequence-map read-table (in-query dbc #:fetch size (sql-ref 'select-racket 'nowhere)))]
                        [else (in-list (map read-by-pks (query-rows dbc (sql-ref 'select-rowid 'nowhere))))]))

                (define (update-table [dbc : Connection] [selves : (U Table (Listof Table))]
                                      #:check-first? [check? : Boolean #true]) : Void
                  (define (mksql [ensure? : Boolean]) : (-> Virtual-Statement)
                    (thunk (cond [(not ensure?) (update.sql dbtable '(pkey ...) eam '(column ...))]
                                 [else (simple-select.sql 'ckrowid dbtable '(pkey ...) eam '(column ...))])))
                  (cond [(and view?) (throw exn:fail:unsupported 'update-table "cannot update records of a temporary view")]
                        [else (let ([dbsys (dbsystem-name (connection-dbsystem dbc))])
                                (define up.sql : Statement (table.sql 'update (mksql #false)))
                                (define check.sql : Statement (table.sql 'check-rowid (mksql #true)))
                                (for ([self (if (list? selves) (in-list selves) (in-value selves))])
                                  (let ([rowid : SQL-Datum (table-rowid self)] ...)
                                    (when (and check? (false? (query-maybe-value dbc check.sql rowid ...)))
                                      (schema-throw [exn:schema 'norow `((struct . table) (record . ,(vector rowid ...)))]
                                                    'update-table "no such record found in the table"))
                                    (let ([column-id : SQL-Datum (racket->sql (table-column self) dbsys)] ...)
                                      (cond [(false? eam) (query dbc up.sql column-id ... rowid ...)]
                                            [else (query dbc up.sql column-id ... (serialize self) rowid ...)])))))]))))]))

(define-syntax (define-schema stx)
  (syntax-parse stx
    [(_ Table-Datum (define-table id #:as ID rest ...) ...)
     #'(begin (define-type Table-Datum (U ID ...))
              (define-table id #:as ID rest ...) ...)]))
